namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks native relationship cleanup and interoperability when deleting a linked table.</summary>
public sealed class LinkedTableRelationshipDaoTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(32)]
    [InlineData(64)]
    public Task Dao36_WriterDropMatchesNativeRelationshipCleanupAndSurvivesCompact(int version)
        => AssertNativeAsync(version, useDao36: true);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(64)]
    [InlineData(128)]
    public Task Dao120_WriterDropMatchesNativeRelationshipCleanupAndSurvivesCompact(int version)
        => AssertNativeAsync(version, useDao36: false);

    private static async Task AssertNativeAsync(int version, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.LinkedDropRelationships", databaseExtension: version == 128 ? ".accdb" : ".mdb");
        string sourcePath = session.CreateDatabasePath("linked-source");
        string controlPath = session.CreateDatabasePath("native-control");
        string compactedControl = session.CreateDatabasePath("native-control-compact");
        string producer = $$"""
            $source = $engine.CreateDatabase({{Literal(sourcePath)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $source.Execute('CREATE TABLE [T] ([Id] LONG CONSTRAINT [PK] PRIMARY KEY)', 128)
                $source.Execute('INSERT INTO [T] VALUES (7)', 128)
            } finally { $source.Close() }
            $db = $engine.CreateDatabase({{Literal(session.SourcePath)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE [Local] ([Id] LONG CONSTRAINT [PK] PRIMARY KEY)', 128)
                $db.Execute('INSERT INTO [Local] VALUES (1)', 128)
                $link = $db.CreateTableDef('Linked')
                $link.Connect = ';DATABASE=' + {{Literal(sourcePath)}}
                $link.SourceTableName = 'T'
                $db.TableDefs.Append($link)
                $relation = $db.CreateRelation('RelatedLink', 'Local', 'Linked', 2)
                $field = $relation.CreateField('Id')
                $field.ForeignName = 'Id'
                $relation.Fields.Append($field)
                $db.Relations.Append($relation)
                Write-Output ('BEFORE=' + $db.Relations('RelatedLink').Name)
            } finally { $db.Close() }
            """;
        Assert.Contains("BEFORE=RelatedLink", RunNative(producer, session, useDao36), StringComparison.Ordinal);
        File.Copy(session.SourcePath, controlPath);
        byte[] sourceBefore = await File.ReadAllBytesAsync(sourcePath, Ct);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new() { UseLockFile = false }, Ct))
        {
            await writer.DropTableAsync("Linked", Ct);
        }

        Assert.Equal(sourceBefore, await File.ReadAllBytesAsync(sourcePath, Ct));
        string control = $$"""
            function Check-Dropped([string]$path) {
                $db = $engine.OpenDatabase($path, $false, $true)
                try {
                    foreach ($table in $db.TableDefs) { if ($table.Name -eq 'Linked') { throw 'Deleted link survived' } }
                    foreach ($relation in $db.Relations) { if ($relation.Name -eq 'RelatedLink') { throw 'Deleted link relationship survived' } }
                    $rows = $db.OpenRecordset('Local', 1)
                    try {
                        if ($rows.EOF -or $rows.Fields('Id').Value -ne 1) { throw 'Unrelated local row changed' }
                        $rows.MoveNext()
                        if (!$rows.EOF) { throw 'Unexpected unrelated local row' }
                    } finally { $rows.Close() }
                } finally { $db.Close() }
            }
            $db = $engine.OpenDatabase({{Literal(controlPath)}})
            try {
                $db.TableDefs.Delete('Linked')
                $db.Relations.Refresh()
                foreach ($relation in $db.Relations) { if ($relation.Name -eq 'RelatedLink') { throw 'Native delete kept linked relationship' } }
            } finally { $db.Close() }
            Check-Dropped {{Literal(controlPath)}}
            Check-Dropped {{Literal(session.SourcePath)}}
            $engine.CompactDatabase({{Literal(controlPath)}}, {{Literal(compactedControl)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            $engine.CompactDatabase({{Literal(session.SourcePath)}}, {{Literal(session.CompactedPath)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            Check-Dropped {{Literal(compactedControl)}}
            Check-Dropped {{Literal(session.CompactedPath)}}
            Write-Output 'NATIVE_LINK_DROP_AND_COMPACT=OK'
            """;
        Assert.Contains("NATIVE_LINK_DROP_AND_COMPACT=OK", RunNative(control, session, useDao36), StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new() { UseLockFile = false }, Ct);
        Assert.Empty(await reader.ListLinkedTablesAsync(Ct));
        Assert.Empty(await reader.ListRelationshipsAsync(Ct));
    }

    private static string RunNative(string script, AccessRoundTripSession session, bool useDao36)
    {
        if (useDao36)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(1));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
            return result.StandardOutput;
        }

        AccessRoundTripEnvironment.CompactResult dao = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(1));
        Assert.True(dao.ExitCode == 0, dao.StdOut + dao.StdErr);
        return dao.StdOut;
    }

    private static string Literal(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);
}
