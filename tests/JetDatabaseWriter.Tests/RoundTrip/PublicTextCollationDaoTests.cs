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
/// <param name="output">Receives native engine and module diagnostics.</param>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class PublicTextCollationDaoTests(ITestOutputHelper output)
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
        => this.VerifyAsync(format, collation, useDao36: false);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet3Mdb, TextCollation.General97)]
    [InlineData(DatabaseFormat.Jet4Mdb, TextCollation.GeneralLegacy)]
    public Task Dao36_ExplicitChoice_SurvivesMutationSeekAndCompact(DatabaseFormat format, TextCollation collation)
        => this.VerifyAsync(format, collation, useDao36: true);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task Dao120_NativeAuthoredColumns_CompactTextToHeaderDefaultWhileRetainingMemo()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeCompactCollation");
        File.Copy(TestDatabases.TestV2010, session.SourcePath);
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult authored = session.RunDaoEngineScript(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $db.Execute('CREATE TABLE [NativeCompactControl] ([Code] TEXT(40) CONSTRAINT [PK_Control] PRIMARY KEY, [Note] MEMO)',128)
                $db.Execute("INSERT INTO [NativeCompactControl] VALUES ('caf$([char]0x00E9)','retained')",128)
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(authored.ExitCode == 0, authored.StdOut + authored.StdErr);
        await AssertControlMetadataAsync(session.SourcePath, TextCollation.General, TextCollation.General);
        string control = session.CreateDatabasePath("native-control");
        string controlCompacted = session.CreateDatabasePath("native-control-compacted");
        File.Copy(session.SourcePath, control);

        // Change only the masked database default. All field descriptors, index
        // keys and rows remain the independent native engine's original bytes.
        byte[] mixed = await File.ReadAllBytesAsync(session.SourcePath, Ct);
        int versionOffset = JetFormat.HeaderLayoutOf(DatabaseFormat.AceAccdb).SortVersionOffset;
        Assert.Equal((byte)1, JetFormat.FromHeader(mixed).DefaultTextSortOrder.Version);
        mixed[versionOffset] ^= 1;
        Assert.Equal((byte)0, JetFormat.FromHeader(mixed).DefaultTextSortOrder.Version);
        await File.WriteAllBytesAsync(session.SourcePath, mixed, Ct);
        await AssertControlMetadataAsync(session.SourcePath, TextCollation.GeneralLegacy, TextCollation.General);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            function Check-Control([string]$path) {
                $db=$engine.OpenDatabase($path)
                try {
                    $rs=$db.OpenRecordset('NativeCompactControl',1)
                    try {
                        $rs.Index='PK_Control'
                        $rs.Seek('=',('CAF'+[char]0x00C9))
                        if($rs.NoMatch -or $rs.Fields('Note').Value -ne 'retained') { throw 'Native control seek failed' }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Check-Control {{source}}
            Check-Control {{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(control)}}
            $engine.CompactDatabase({{source}},{{compacted}})
            $engine.CompactDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(control)}},{{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(controlCompacted)}})
            Check-Control {{compacted}}
            Check-Control {{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(controlCompacted)}}
            Write-Output "DAO_ENGINE_VERSION=$($engine.Version)"
            foreach($module in ((Get-Process -Id $PID).Modules | Where-Object { $_.ModuleName -match '^ACE|^dao' })) {
                Write-Output "DAO_MODULE=$($module.FileName)|VERSION=$($module.FileVersionInfo.FileVersion)"
            }
            Write-Output 'NATIVE_COMPACT_COLLATION_CONTROL=OK'
            """,
            TimeSpan.FromMinutes(2));
        output.WriteLine(result.StdOut);
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        Assert.Contains("NATIVE_COMPACT_COLLATION_CONTROL=OK", result.StdOut, StringComparison.Ordinal);
        await AssertControlMetadataAsync(controlCompacted, TextCollation.General, TextCollation.General);
        await AssertControlMetadataAsync(session.CompactedPath, TextCollation.GeneralLegacy, TextCollation.GeneralLegacy);
    }

    private static async Task AssertControlMetadataAsync(string path, TextCollation databaseCollation, TextCollation textCollation)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        Assert.Equal(databaseCollation, reader.Database.Format.DefaultTextSortOrder.Collation);
        foreach (ColumnMetadata column in await reader.GetColumnMetadataAsync("NativeCompactControl", Ct))
        {
            Assert.Equal(column.Name == "Code" ? textCollation : TextCollation.General, column.TextCollation);
        }
    }

    private async Task VerifyAsync(DatabaseFormat format, TextCollation collation, bool useDao36)
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

        TextCollation compactedTextCollation;
        await using (AccessReader sourceReader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            compactedTextCollation = Assert.IsType<TextCollation>(sourceReader.Database.Format.DefaultTextSortOrder.Collation);
            foreach (ColumnMetadata column in await sourceReader.GetColumnMetadataAsync("Collated", Ct))
            {
                Assert.Equal(collation, column.TextCollation);
            }
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
        output.WriteLine(nativeOutput);
        Assert.True(File.Exists(session.CompactedPath));
        await using (AccessReader sourceReader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            foreach (ColumnMetadata column in await sourceReader.GetColumnMetadataAsync("Collated", Ct))
            {
                Assert.Equal(collation, column.TextCollation);
            }
        }

        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        foreach (ColumnMetadata column in await reader.GetColumnMetadataAsync("Collated", Ct))
        {
            Assert.Equal(column.Name == "Renamed" ? compactedTextCollation : collation, column.TextCollation);
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
