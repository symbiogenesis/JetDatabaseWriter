namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Stored table display text follows native DAO metadata preservation during column edits.</summary>
public sealed class NativeDisplayPropertyTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet3Mdb, "rename")]
    [InlineData(DatabaseFormat.Jet3Mdb, "case")]
    [InlineData(DatabaseFormat.Jet3Mdb, "drop")]
    [InlineData(DatabaseFormat.Jet3Mdb, "unrelated-rename")]
    [InlineData(DatabaseFormat.Jet3Mdb, "unrelated-drop")]
    [InlineData(DatabaseFormat.Jet4Mdb, "rename")]
    [InlineData(DatabaseFormat.Jet4Mdb, "case")]
    [InlineData(DatabaseFormat.Jet4Mdb, "drop")]
    [InlineData(DatabaseFormat.Jet4Mdb, "unrelated-rename")]
    [InlineData(DatabaseFormat.Jet4Mdb, "unrelated-drop")]
    public Task Dao36ColumnEdit_PreservesDisplayTextThroughReopenAndCompact(DatabaseFormat format, string operation)
        => RunRoundTripAsync(format, operation, useDao36: true);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("rename")]
    [InlineData("case")]
    [InlineData("drop")]
    [InlineData("unrelated-rename")]
    [InlineData("unrelated-drop")]
    public Task Dao120ColumnEdit_PreservesDisplayTextThroughReopenAndCompact(string operation)
        => RunRoundTripAsync(DatabaseFormat.AceAccdb, operation, useDao36: false);

    private static async Task RunRoundTripAsync(DatabaseFormat format, string operation, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeDisplay", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string control = session.CreateDatabasePath("native-control");
        string controlCompact = session.CreateDatabasePath("native-control-compact");
        string sourceLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        string created = RunEngine(
            $$"""
            $db = $engine.CreateDatabase({{sourceLiteral}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE [T] ([Id] LONG, [Note] LONG, [Keep] LONG)', 128)
                $db.Execute('INSERT INTO [T] ([Id],[Note],[Keep]) VALUES (1,2,9)', 128)
                $table = $db.TableDefs('T')
                $table.Properties.Append($table.CreateProperty('Filter', 12, '[Note]>0'))
                $table.Properties.Append($table.CreateProperty('OrderBy', 12, '[Note] DESC, [Keep]'))
                Write-Output ('ENGINE=' + $engine.Version)
            } finally { $db.Close() }
            """,
            session.WorkDir,
            useDao36);
        Assert.Contains("ENGINE=", created, StringComparison.Ordinal);
        string[] original = await ReadDisplayPropertiesAsync(session.SourcePath);
        await File.WriteAllBytesAsync(control, await File.ReadAllBytesAsync(session.SourcePath, Ct), Ct);

        string nativeEdit = operation switch
        {
            "rename" => "$table.Fields('Note').Name = 'Renamed'",
            "case" => "$table.Fields('Note').Name = 'note'",
            "drop" => "$table.Fields.Delete('Note')",
            "unrelated-rename" => "$table.Fields('Id').Name = 'Renamed'",
            _ => "$table.Fields.Delete('Id')",
        };
        _ = RunEngine(
            $$"""
            $db = $engine.OpenDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(control)}})
            try {
                $table = $db.TableDefs('T')
                {{nativeEdit}}
            } finally { $db.Close() }
            """,
            session.WorkDir,
            useDao36);
        Assert.Equal(original, await ReadDisplayPropertiesAsync(control));
        string[] expectedColumns = await ReadColumnNamesAsync(control);
        _ = RunEngine(ValidateAndCompactScript(control, controlCompact, version, operation), session.WorkDir, useDao36);
        Assert.Equal(original, await ReadDisplayPropertiesAsync(controlCompact));

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            switch (operation)
            {
                case "rename":
                    await writer.RenameColumnAsync("T", "Note", "Renamed", Ct);
                    break;
                case "case":
                    await writer.RenameColumnAsync("T", "Note", "note", Ct);
                    break;
                case "drop":
                    await writer.DropColumnAsync("T", "Note", Ct);
                    break;
                case "unrelated-rename":
                    await writer.RenameColumnAsync("T", "Id", "Renamed", Ct);
                    break;
                default:
                    await writer.DropColumnAsync("T", "Id", Ct);
                    break;
            }
        }

        Assert.Equal(original, await ReadDisplayPropertiesAsync(session.SourcePath));
        Assert.Equal(expectedColumns, await ReadColumnNamesAsync(session.SourcePath));
        _ = RunEngine(ValidateAndCompactScript(session.SourcePath, session.CompactedPath, version, operation), session.WorkDir, useDao36);
        Assert.Equal(original, await ReadDisplayPropertiesAsync(session.CompactedPath));
        Assert.Equal(expectedColumns, await ReadColumnNamesAsync(session.CompactedPath));
        await using AccessReader compacted = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        Assert.Equal(1, await compacted.GetRealRowCountAsync("T", Ct));
    }

    private static string ValidateAndCompactScript(string source, string compacted, int version, string operation)
    {
        int expectedColumns = operation.EndsWith("drop", StringComparison.Ordinal) ? 2 : 3;
        return $$"""
            $db = $engine.OpenDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(source)}})
            try {
                if ($db.TableDefs('T').Fields.Count -ne {{expectedColumns}}) { throw 'Wrong column count.' }
                if ($db.TableDefs('T').Properties('Filter').Value -cne '[Note]>0') { throw 'Changed Filter.' }
                if ($db.TableDefs('T').Properties('OrderBy').Value -cne '[Note] DESC, [Keep]') { throw 'Changed OrderBy.' }
                $rs = $db.OpenRecordset('SELECT [Keep] FROM [T]')
                try { if ($rs.EOF -or $rs.Fields('Keep').Value -ne 9) { throw 'Lost surviving data.' } } finally { $rs.Close() }
                $db.Execute('INSERT INTO [T] ([Keep]) VALUES (8)', 128)
                $db.Execute('UPDATE [T] SET [Keep]=7 WHERE [Keep]=8', 128)
                $db.Execute('DELETE FROM [T] WHERE [Keep]=7', 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(source)}}, {{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(compacted)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            Write-Output 'NATIVE_COMPACT_OK'
            """;
    }

    private static string RunEngine(string script, string directory, bool useDao36)
    {
        if (useDao36)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, directory, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO36 failed: {result.StandardOutput}\n{result.StandardError}");
            return result.StandardOutput;
        }

        AccessRoundTripEnvironment.CompactResult ace = AccessRoundTripEnvironment.RunDaoScript(
            $$"""
            $ErrorActionPreference = 'Stop'
            $engine = New-Object -ComObject DAO.DBEngine.120
            try {
                {{script}}
            } finally { [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null }
            """,
            directory,
            TimeSpan.FromMinutes(2));
        Assert.True(ace.ExitCode == 0, $"DAO120 failed: {ace.StdOut}\n{ace.StdErr}");
        return ace.StdOut;
    }

    private static async Task<string[]> ReadColumnNamesAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        return (await reader.GetColumnMetadataAsync("T", Ct)).Select(column => column.Name).ToArray();
    }

    private static async Task<string[]> ReadDisplayPropertiesAsync(string path)
    {
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        long page = Assert.Single(await harness.Services.Catalog.GetUserTablesAsync(Ct), table => table.Name == "T").TDefPage;
        ColumnPropertyBlock properties = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Catalog.ReadLvPropForTableAsync(page, Ct));
        ColumnPropertyTarget target = Assert.IsType<ColumnPropertyTarget>(properties.FindTableTarget());
        string[] result = target.Entries.Where(entry => entry.Name is "Filter" or "OrderBy")
            .Select(entry => $"{entry.Name}|{entry.DataType}|{entry.DdlFlag}|{Convert.ToHexString(entry.Value)}").ToArray();
        Assert.Equal(2, result.Length);
        return result;
    }
}
