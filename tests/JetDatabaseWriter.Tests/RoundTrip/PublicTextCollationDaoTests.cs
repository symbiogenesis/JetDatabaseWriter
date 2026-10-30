namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native engines read, seek, mutate and compact explicitly chosen column collations.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class PublicTextCollationDaoTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb, TextCollation.GeneralLegacy)]
    [InlineData(DatabaseFormat.Jet4Mdb, TextCollation.General)]
    [InlineData(DatabaseFormat.AceAccdb, TextCollation.GeneralLegacy)]
    [InlineData(DatabaseFormat.AceAccdb, TextCollation.General)]
    public Task Dao120_ExplicitChoice_SurvivesMutationSeekAndCompact(DatabaseFormat format, TextCollation collation)
        => VerifyAsync(format, collation, useDao36: false);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet3Mdb, TextCollation.General97)]
    [InlineData(DatabaseFormat.Jet4Mdb, TextCollation.GeneralLegacy)]
    public Task Dao36_ExplicitChoice_SurvivesMutationSeekAndCompact(DatabaseFormat format, TextCollation collation)
        => VerifyAsync(format, collation, useDao36: true);

    private static async Task VerifyAsync(DatabaseFormat format, TextCollation collation, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.PublicTextCollation", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, format, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("Collated", [new ColumnDefinition("Code", typeof(string), 40) { IsPrimaryKey = true, TextCollation = collation }, new ColumnDefinition("Discard", typeof(int))], Ct);
            await writer.InsertRowAsync("Collated", ["caf\u00e9", 9], Ct);
            await writer.AddColumnAsync("Collated", new ColumnDefinition("Note", typeof(string)) { TextCollation = collation }, Ct);
            Assert.Equal(1, await writer.UpdateRowsAsync("Collated", RowCriteria.Where("Code", "caf\u00e9"), new RowValues { ["Note"] = "writer" }, Ct));
            await writer.DropColumnAsync("Collated", "Discard", Ct);
            await writer.RenameColumnAsync("Collated", "Code", "Renamed", Ct);
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $rs=$db.OpenRecordset('Collated',1)
                try {
                    $rs.Index=$db.TableDefs.Item('Collated').Indexes.Item(0).Name
                    $rs.Seek('=',('CAF'+[char]0x00C9))
                    if($rs.NoMatch -or $rs.Fields('Note').Value -ne 'writer') { throw 'Native seek missed writer key or Memo value' }
                    $rs.Edit()
                    $rs.Fields('Note').Value='native'
                    $rs.Update()
                } finally { $rs.Close() }
                $rejected=$false
                try { $db.Execute("INSERT INTO [Collated] ([Renamed]) VALUES ('CAF$([char]0x00C9)')",128) } catch {
                    if(@($engine.Errors | ForEach-Object { $_.Number }) -notcontains 3022) { throw }
                    $rejected=$true
                }
                if(!$rejected) { throw 'Native engine accepted a duplicate collated key' }
                $db.Execute("INSERT INTO [Collated] ([Renamed],[Note]) VALUES ('second','inserted')",128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}},{{compacted}})
            $db=$engine.OpenDatabase({{compacted}})
            try {
                $rs=$db.OpenRecordset('Collated',1)
                try {
                    $rs.Index=$db.TableDefs.Item('Collated').Indexes.Item(0).Name
                    $rs.Seek('=',('CAF'+[char]0x00C9))
                    if($rs.NoMatch -or $rs.Fields('Note').Value -ne 'native') { throw 'Native compact lost writer key or native update' }
                    $rs.Seek('=','SECOND')
                    if($rs.NoMatch -or $rs.Fields('Note').Value -ne 'inserted') { throw 'Native compact lost inserted key' }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Write-Output 'EXPLICIT_COLLATION_OK'
            """;
        string nativeOutput;
        if (useDao36)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
            nativeOutput = result.StandardOutput;
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
            nativeOutput = result.StdOut;
        }

        Assert.Contains("EXPLICIT_COLLATION_OK", nativeOutput, StringComparison.Ordinal);
        Assert.True(File.Exists(session.CompactedPath));
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        foreach (ColumnMetadata column in await reader.GetColumnMetadataAsync("Collated", Ct))
        {
            Assert.Equal(collation, column.TextCollation);
        }

        int rows = 0;
        await foreach (object[] row in reader.Rows("Collated", cancellationToken: Ct))
        {
            string key = Assert.IsType<string>(row[0]);
            Assert.True(key is "caf\u00e9" or "second");
            Assert.Equal(key == "caf\u00e9" ? "native" : "inserted", row[1]);
            rows++;
        }

        Assert.Equal(2, rows);
    }
}
